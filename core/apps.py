# -*- coding: utf-8 -*-
from __future__ import unicode_literals

# generic
import logging
import os

# combine
from django.apps import AppConfig
from django.conf import settings

# Get an instance of a logger
logger = logging.getLogger(__name__)

# NOTE: manual configuration of core app not currently used, but leaving if needed
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

		logger.debug('Core application ready method preparations firing')

		# create home working directory
		self.create_home_working_directory()


	def create_home_working_directory(self):

		'''
		Method to create directory /home/combine/data/combine if does not exist
		'''

		# parse home working directory
		hwd = settings.BINARY_STORAGE.split('file://')[-1]

		# create if not exists
		if not os.path.exists(hwd):
			os.makedirs(hwd)
