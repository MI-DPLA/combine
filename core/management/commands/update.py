
# generic imports
import datetime
import logging
import os
import pdb
import time

# django
from django.core.management.base import BaseCommand, CommandError

# import core
from core.models import *

# Get an instance of a logger
logger = logging.getLogger(__name__)

class Command(BaseCommand):

	'''
	Manage command to update Combine.

	Performs the following:
		- pull from github, updates all branches
		- if relase passed, checkout release/branch
		- pip install requirements
		- collect static django
		- restart gunicorn, livy session, celery
	'''
	
	help = 'Update Combine'


	# python path
	PYTHON_PATH = '/usr/local/anaconda/envs/combine/bin'

	
	def add_arguments(self, parser):

		# add optional organization ids to skip
		parser.add_argument(
			'--release',
			dest='release',
			help='GitHub branch/release to update to',
			type=str			
		)

	
	def handle(self, *args, **options):

		'''
		Perform series of OS commands as sudo user
		'''

		logger.debug('Updating Combine')

		# git pull		
		os.system('git pull')

		# checkout release if provided		
		if options.get('release', None) != None:
			release = options['release']
			logger.debug('release/branch provided, checking out: %s' % release)

			# git checkout			
			os.system('git checkout %s' % release)

		# install requirements as combine user		
		os.system('%s/pip install -r requirements.txt' % (self.PYTHON_PATH))

		# collect django static
		os.system('%s/python manage.py collectstatic --noinput' % (self.PYTHON_PATH))

		# restart gunicorn
		self._restart_gunicorn()

		# restart livy and livy session
		self._restart_livy()

		# restart celery background tasks
		self._restart_celery()

		# return
		self.stdout.write(self.style.SUCCESS('Update complete.'))


	def _restart_gunicorn(self):
		
		# get supervisor handle
		sp = SupervisorRPCClient()
		# fire action	
		results = sp.restart_process('gunicorn') 
		logger.debug(results)


	def _restart_livy(self):

		# get supervisor handle
		sp = SupervisorRPCClient()
		# fire action	
		results = sp.restart_process('livy') 
		logger.debug(results)

		# sleep
		time.sleep(10)

		# get active livy sessions - restart or start
		active_ls = LivySession.get_active_session()		
		if not active_ls:
			logger.debug('active livy session not found, starting')
			livy_session = LivySession()
			livy_session.start_session()
		else:
			logger.debug('single, active session found, and restart flag passed, restarting')			
			new_ls = active_ls.restart_session()


	def _restart_celery(self):

		# get supervisor handle
		sp = SupervisorRPCClient()
		# fire action	
		results = sp.restart_process('celery') 
		logger.debug(results)



