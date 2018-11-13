
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
		self._run_cmd_as_combine_user("git pull")

		# checkout release if provided		
		if options.get('release', None) != None:
			release = options['release']
			logger.debug('release/branch provided, checking out: %s' % release)

			# git checkout			
			self._run_cmd_as_combine_user('git checkout %s' % release)

		# install requirements as combine user		
		self._run_cmd_as_combine_user("%s/pip install -r requirements.txt" % (self.PYTHON_PATH))

		# collect django static
		self._run_cmd_as_combine_user("%s/python manage.py collectstatic --noinput" % (self.PYTHON_PATH))

		# restart gunicorn
		os.system('supervisorctl restart gunicorn')

		# restart livy and livy session
		os.system('supervisorctl restart livy')
		time.sleep(10)
		# get active livy sessions
		active_ls = LivySession.get_active_session()

		# none found
		if not active_ls:
			logger.debug('active livy session not found, starting')
			livy_session = LivySession()
			livy_session.start_session()

		else:
			logger.debug('single, active session found, and restart flag passed, restarting')			
			new_ls = active_ls.restart_session()

		# restart celery background tasks
		os.system('supervisorctl restart celery')
		time.sleep(10)
		# get supervisor handle
		sp = SupervisorRPCClient()
		# fire action	
		results = sp.restart_process('celery') 
		logger.debug(results)

		# return
		self.stdout.write(self.style.SUCCESS('Update complete.'))


	def _run_cmd_as_combine_user(self, cmd_str):

		'''
		Helper function to run command as Combine user		
		'''

		os.system('su -c "%s" combine' % cmd_str)