
# generic imports
import datetime
import logging
import os
from packaging import version
import pdb
import shutil
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

		# release
		parser.add_argument(
			'--release',
			dest='release',
			help='GitHub branch/release to update to',
			type=str,
			default=None
		)

		# update method
		parser.add_argument(
			'--run_update_snippet',
			dest='run_update_snippet',
			help='Update code snippet to run',
			type=str,
			default=None
		)

		# update method
		parser.add_argument(
			'--run_update_snippets_only',
			action='store_true',
			help='Run update snippets only during update'
		)


	def handle(self, *args, **options):

		'''
		Handler for updates to Combine
		'''

		logger.debug('Updating Combine')

		# run update snippet if passed
		if options.get('run_update_snippet'):
			self.run_update_snippet(args, options)

		# else, run update
		else:
			self.update(args, options)


	def update(self, args, options):

		'''
		Method to handle branch/tagged release update
		'''

		# do not run at all if Combine is Docker deployed
		if getattr(settings,'COMBINE_DEPLOYMENT','server') != 'docker':

			# if not running update snippets only
			if not options.get('run_update_snippets_only', False):

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

				# ensure redis version
				os.system('%s/pip uninstall redis -y' % (self.PYTHON_PATH))
				os.system('%s/pip install redis==2.10.6' % (self.PYTHON_PATH))

				# collect django static
				os.system('%s/python manage.py collectstatic --noinput' % (self.PYTHON_PATH))

				# restart gunicorn
				self._restart_gunicorn()

				# restart livy and livy session
				self._restart_livy()

				# restart celery background tasks
				self._restart_celery()

			# run update code snippets
			vuh = VersionUpdateHelper()
			vuh.run_update_snippets()

			# return
			self.stdout.write(self.style.SUCCESS('Update complete.'))

		# docker return
		else:
			self.stdout.write(self.style.ERROR('Update script does not currently support Docker deployment.'))


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


	def run_update_snippet(self, args, options):

		'''
		Method to run update snippet if passed
		'''

		# init VersionUpdateHelper instance
		vuh = VersionUpdateHelper()

		# get snippet
		snippet = getattr(vuh, options.get('run_update_snippet'), None)
		if snippet != None:
			snippet()
		else:
			logger.debug('Update snippet "%s" could not be found' % options.get('run_update_snippet', None))



class VersionUpdateHelper(object):

	'''
	Class to manage actions specific to version-to-version updates
	'''


	def __init__(self):

		# registered, ordered list of snippets
		self.registered_snippets = [
			self.v0_4__set_job_baseline_combine_version,
			self.v0_4__update_transform_job_details,
			self.v0_4__set_job_current_combine_version,
		]


	def run_update_snippets(self):

		'''
		Method to loop through update snippets and fire
		'''

		for snippet in self.registered_snippets:
			try:
				snippet()
			except Exception as e:
				logger.debug('Could not run udpate snippet: %s' % snippet.__name__)
				logger.debug(str(e))


	def v0_4__set_job_baseline_combine_version(self):

		'''
		Method to set combine_version as v0.1 in job_details for all lacking version
		'''

		logger.debug('v0_4__set_job_baseline_combine_version: setting Job combine_version to v0.1 if not set')

		# get Transform Jobs
		jobs = Job.objects.all()

		# loop through jobs
		for job in jobs:

			# check for combine_version key
			if not job.job_details_dict.get('combine_version', False):

				logger.debug('stamping v0.1 combine_version to Job: %s' % (job))

				# update job_details
				job.update_job_details({'combine_version':'v0.1'})


	def v0_4__set_job_current_combine_version(self):

		'''
		Method to set combine_version as current Combine version in job_details
		'''

		logger.debug('v0_4__set_job_current_combine_version: checking and setting Job combine_version to %s' % (settings.COMBINE_VERSION))

		# get Transform Jobs
		jobs = Job.objects.all()

		# loop through jobs
		for job in jobs:

			# compare and stamp
			if version.parse(job.job_details_dict['combine_version']) < version.parse(settings.COMBINE_VERSION):

				logger.debug('stamping %s combine_version to Job: %s' % (settings.COMBINE_VERSION, job))

				# update job_details
				job.update_job_details({'combine_version':settings.COMBINE_VERSION})


	def v0_4__update_transform_job_details(self):

		'''
		Method to update job_details for Transform Jobs if from_v < v0.4 or None
		'''

		logger.debug('v0_4__update_transform_job_details: updating job details for pre v0.4 Transform Jobs')

		# get Transform Jobs
		trans_jobs = Job.objects.filter(job_type='TransformJob')

		# loop through and check for single Transformation Scenario
		for job in trans_jobs:

			# check version
			if version.parse(job.job_details_dict['combine_version']) < version.parse('v0.4'):

				logger.debug('Transform Job "%s" is Combine version %s, checking if needs updating' % (job, job.job_details_dict['combine_version']))

				# check for 'transformation' key in job_details
				if job.job_details_dict.get('transformation', False):

					# get transform details
					trans_details = job.job_details_dict.get('transformation')

					# check for 'id' key at this level, indicating < v0.4
					if 'id' in trans_details.keys():

						logger.debug('Transform Job "%s" requires job details updating, performing' % job)

						# create dictionary
						trans_dict = {
							'scenarios':[
								{
									'id':trans_details['id'],
									'name':trans_details['name'],
									'type':trans_details['type'],
									'type_human':trans_details['type'],
									'index':0
								}
							],
							'scenarios_json':'[{"index":0,"trans_id":%s}]' % trans_details['id']
						}

						# update job_details
						job.update_job_details({'transformation':trans_dict})














