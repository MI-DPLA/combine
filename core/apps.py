# -*- coding: utf-8 -*-
from __future__ import unicode_literals

# generic
import logging

# combine
from django.apps import AppConfig

# Get an instance of a logger
logger = logging.getLogger(__name__)

# NOTE: manual configuration of core app not currently used, but leaving if needed
# class CoreConfig(AppConfig):
	
# 	name = 'core'

# 	def ready(self):

# 		'''
# 		ready() method fires once, when application is loaded and ready
# 		https://docs.djangoproject.com/en/dev/ref/applications/#django.apps.AppConfig.ready

# 		This fires any functions defined here that are needed when Combine starts.

# 		Args:
# 			(django.apps.AppConfig): instance of 'Core' application config

# 		Returns:
# 			None
# 		'''

# 		# logger.debug('Core application ready method preperations firing')

# 		pass


