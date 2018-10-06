import subprocess

from django.conf import settings
from django.db.models.query import QuerySet
from core.models import LivySession, SupervisorRPCClient, CombineBackgroundTask


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
		if type(lv) == LivySession:
			# refresh single session
			lv.refresh_from_livy()
		elif type(lv) == QuerySet:
			# multiple Combine LivySession founds, loop through
			for s in lv:
				s.refresh_from_livy()
		else:
			pass

	return {
		'LIVY_SESSION':lv
	}


def bgtasks_proc(request):

	'''
	Get status of Background Tasks
		- improvements would be to determine if task running, but would require DB query
	'''

	# try:

	# get supervisor and status
	sp = SupervisorRPCClient()
	proc = sp.check_process('celery')

	# check for uncompleted CombineBackgroundTask instances
	for ct in CombineBackgroundTask.objects.filter(completed=False):
		ct.update()
	
	if CombineBackgroundTask.objects.filter(completed=False).count() > 0:
	 	
	 	# set status
		bg_tasks_busy = True

	else:
		bg_tasks_busy = False		

	# return
	return {
		'BGTASKS_PROC':proc,
		'BGTASKS_BUSY':bg_tasks_busy
	}

	# except:
	# 	pass


def combine_git_info(request):

	'''
	Return branch of combine git repo
	'''

	# attempt to read tag
	tag_query = subprocess.Popen("git describe --tags", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
	out = tag_query.stdout.read().decode('utf-8').rstrip("\n")
	err = tag_query.stderr.read().decode('utf-8').rstrip("\n")

	# if tag found, use
	if out != '':
		branch = out

	# else, read branch 
	else:
		branch = subprocess.Popen("git rev-parse --abbrev-ref HEAD", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE).stdout.read().decode('utf-8').rstrip("\n")

	# return
	return {'COMBINE_GIT_BRANCH':branch}















