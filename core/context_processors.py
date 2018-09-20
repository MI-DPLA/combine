import subprocess

from django.conf import settings
from core.models import LivySession, SupervisorRPCClient


def combine_settings(request):
	
	'''
	Make some settings variables available to all templates
	'''

	return {
		'APP_HOST': settings.APP_HOST,		
		'DPLA_API_KEY': settings.DPLA_API_KEY,
		'OAI_RESPONSE_SIZE':settings.OAI_RESPONSE_SIZE,
		'COMBINE_OAI_IDENTIFIER':settings.COMBINE_OAI_IDENTIFIER,
		'INCLUDE_ATTRIBUTES_GENERIC_MAPPER':settings.INCLUDE_ATTRIBUTES_GENERIC_MAPPER
	}


def livy_session(request):

	'''
	Make Livy session information available to all views
	'''

	# get active livy session
	lv = LivySession.get_active_session()
	if lv:
		lv.refresh_from_livy()

	return {
		'LIVY_SESSION':lv
	}


def bgtasks_proc(request):

	'''
	Get status of Background Tasks
		- improvements would be to determine if task running, but would require DB query
	'''

	try:
		# get supervisor and status
		sp = SupervisorRPCClient()
		proc = sp.check_process('combine_background_tasks')

		# return
		return {
			'BGTASKS_PROC':proc
		}

	except:
		pass


def combine_git_info(request):

	'''
	Return branch of combine git repo
	'''

	branch = subprocess.Popen("git rev-parse --abbrev-ref HEAD", shell=True, stdout=subprocess.PIPE).stdout.read().decode('utf-8').rstrip("\n")
	return {'COMBINE_GIT_BRANCH':branch}