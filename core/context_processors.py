from django.conf import settings
from core.models import LivySession

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