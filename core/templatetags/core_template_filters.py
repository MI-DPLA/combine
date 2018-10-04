
import logging
import re
from django import template
from django.conf import settings

numeric_test = re.compile("^\d+$")
register = template.Library()

# Get an instance of a logger
logger = logging.getLogger(__name__)


def get_obj_attr(value, arg):
	'''
	Gets an attribute of an object dynamically from a string name
	https://stackoverflow.com/questions/844746/performing-a-getattr-style-lookup-in-a-django-template
	'''

	if hasattr(value, str(arg)):
		return getattr(value, arg)
	elif hasattr(value, 'has_key') and value.has_key(arg):
		return value[arg]
	elif numeric_test.match(str(arg)) and len(value) > int(arg):
		return value[int(arg)]
	else:
		# return settings.TEMPLATE_STRING_IF_INVALID
		return None


def get_dict_value(dictionary, key):
	
	'''
	Return value from dictionary with variable key
	'''

	return dictionary.get(key, False)


def es_field_name_format(field_name):

	'''
	Template filter to convert ES friendly field names
	into human friendly XML-like paths
	'''

	# add slashes
	field_name = re.sub('\|','/',field_name)

	return '/%s' % field_name


register.filter('get_obj_attr', get_obj_attr)
register.filter('get_dict_value', get_dict_value)
register.filter('es_field_name_format', es_field_name_format)


