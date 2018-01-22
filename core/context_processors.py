from django.conf import settings
from core.models import LivySession

def combine_settings(request):
	
	'''
	Make some settings variables available to all templates
	'''

	return {
		'APP_HOST': settings.APP_HOST,
		'COMBINE_OAI_ENDPOINT': settings.COMBINE_OAI_ENDPOINT,
		'DPLA_API_KEY': settings.DPLA_API_KEY
	}


def livy_session(request):

	'''
	Make Livy session information available to all views
	'''

	# get active livy session
	lv = LivySession.get_active_session()

	return {
		'LIVY_SESSION':lv
	}