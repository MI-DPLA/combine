
# generic imports
from lxml import etree



class PythonUDFRecord(object):

	'''
	Simple class to provide an object with parsed metadata for user defined functions
	'''

	def __init__(self, row):

		# row
		self._row = row

		# get combine id
		self.id = row.id

		# get record id
		self.record_id = row.record_id

		# document string
		self.document = row.document

		# parse XML string, save
		self.xml = etree.fromstring(self.document)

		# get namespace map, popping None values
		_nsmap = self.xml.nsmap.copy()
		try:
			_nsmap.pop(None)
		except:
			pass
		self.nsmap = _nsmap