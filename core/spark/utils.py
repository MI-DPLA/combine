
# generic imports
import django
from lxml import etree
import os
import sys


# check for registered apps signifying readiness, if not, run django.setup() to run as standalone
if not hasattr(django, 'apps'):
	os.environ['DJANGO_SETTINGS_MODULE'] = 'combine.settings'
	sys.path.append('/opt/combine')
	django.setup()	

# import django settings
from django.conf import settings
from django.db import connection



def refresh_django_db_connection():
	
	'''
	Function to refresh connection to Django DB.
	
	Behavior with python files uploaded to Spark context via Livy is atypical when
	it comes to opening/closing connections with MySQL.  Specifically, if jobs are run farther 
	apart than MySQL's `wait_timeout` setting, it will result in the error, (2006, 'MySQL server has gone away').

	Running this function before jobs ensures that the connection is fresh between these python files
	operating in the Livy context, and Django's DB connection to MySQL.

	Args:
		None

	Returns:
		None
	'''

	connection.close()
	connection.connect()



class PythonUDFRecord(object):

	'''
	Class to provide a slim-downed version of core.models.Record that is used for spark UDF functions,
	and for previewing python based validations and transformations
	'''

	def __init__(self, record_input, non_row_input=False, record_id=None, document=None):

		'''
		Instantiated in one of two ways
			1) from a DB row representing a Record in its entirety
			2) manually passed record_id or document (or both), triggered by non_row_input Flag
				- for example, this is used for testing record_id transformations
		'''

		if non_row_input:

			# if record_id provided, set
			if record_id:
				self.record_id = record_id

			# if document provided, set and parse
			if document:
				self.document = document

				try:

					# parse XML string, save
					self.xml = etree.fromstring(self.document.encode('utf-8'))

					# get namespace map, popping None values
					_nsmap = self.xml.nsmap.copy()
					try:
						_nsmap.pop(None)
					except:
						pass
					self.nsmap = _nsmap

				except:

					self.xml = None
					self.nsmap = None

		else:

			# row
			self._row = record_input

			# get db id
			try:
				self.id = self._row._id
			except:
				pass

			# get record id
			self.record_id = self._row.record_id

			# document string
			self.document = self._row.document

			# set error
			self.error = self._row.error

			try:

				# parse XML string, save
				self.xml = etree.fromstring(self.document.encode('utf-8'))

				# get namespace map, popping None values
				_nsmap = self.xml.nsmap.copy()
				try:
					_nsmap.pop(None)
				except:
					pass
				self.nsmap = _nsmap

				# set inverted nsmap
				self.nsmap_inv = {v: k for k, v in self.nsmap.items()}

			except:

				self.xml = None
				self.nsmap = None

			
