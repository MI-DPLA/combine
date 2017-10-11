from django.conf import settings


def combine_settings(request):
	
	'''
	Make some settings variables available to all templates
	'''

	return {
		'APP_HOST': settings.APP_HOST,
		'COMBINE_OAI_ENDPOINT': settings.COMBINE_OAI_ENDPOINT
	}