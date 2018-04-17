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

		logger.debug('Core application ready method preperations firing')

		# write s3 credentials
		self.set_s3_credentials()


	def set_s3_credentials(self):

		'''
		Method to set AWS credentials for S3 bucket access
		at ~/.aws/credentials
		'''

		# aws credentials dir
		aws_creds_dir = '/home/combine/.aws'

		# check if directory exists
		if not os.path.exists(aws_creds_dir):
			os.makedirs(aws_creds_dir)

		with open('%s/credentials' % aws_creds_dir, 'w') as f:
			f.write('''[default]
aws_access_key_id = %s
aws_secret_access_key = %s
''' % (settings.AWS_ACCESS_KEY_ID, settings.AWS_SECRET_ACCESS_KEY))

		


