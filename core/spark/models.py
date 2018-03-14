
# generic imports
from lxml import etree



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

				# parse XML string, save
				self.xml = etree.fromstring(self.document)

				# get namespace map, popping None values
				_nsmap = self.xml.nsmap.copy()
				try:
					_nsmap.pop(None)
				except:
					pass
				self.nsmap = _nsmap

		else:
			# row
			self._row = record_input

			# get combine id
			self.id = self._row.id

			# get record id
			self.record_id = self._row.record_id

			# document string
			self.document = self._row.document

			# parse XML string, save
			self.xml = etree.fromstring(self.document)

			# get namespace map, popping None values
			_nsmap = self.xml.nsmap.copy()
			try:
				_nsmap.pop(None)
			except:
				pass
			self.nsmap = _nsmap