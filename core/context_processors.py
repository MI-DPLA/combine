
# general
from core.celery import celery_app
from django.conf import settings
from django.db.models.query import QuerySet
import subprocess

# combine
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
		'COMBINE_DEPLOYMENT':settings.COMBINE_DEPLOYMENT
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

	#####################################################################################################################
	# # SUPERVISOR BASED
	# # get supervisor and status
	# sp = SupervisorRPCClient()

	# # proc = sp.check_process('celery')
	# proc = None

	# # check for uncompleted CombineBackgroundTask instances
	# for ct in CombineBackgroundTask.objects.filter(completed=False):
	# 	ct.update()

	# if CombineBackgroundTask.objects.filter(completed=False).count() > 0:

	#  	# set status
	# 	bg_tasks_busy = True

	# else:

	# 	bg_tasks_busy = False

	# # return
	# return {
	# 	'BGTASKS_PROC':proc,
	# 	'BGTASKS_BUSY':bg_tasks_busy
	# }
	#####################################################################################################################

	#####################################################################################################################
	# CELERY INSPECT
	# https://stackoverflow.com/a/8522470/1196358

	# celery_stats = celery_app.control.inspect().stats()
	
	# return {
	# 	'BGTASKS_PROC':celery_stats
	# }
	#####################################################################################################################

	return {}


def combine_git_info(request):

	'''
	Return state of HEAD for Combine git repo
	'''

	# one liner for branch or tag
	git_head = subprocess.Popen('head_name="$(git symbolic-ref HEAD 2>/dev/null)" || head_name="$(git describe --tags)"; echo $head_name', shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE).stdout.read().decode('utf-8').rstrip("\n")
	if "/" in git_head:
		git_head = git_head.split('/')[-1]

	# return
	return {'COMBINE_GIT_BRANCH':git_head}















