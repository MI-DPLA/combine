
from lxml import etree
import xmltodict


class BaseMapper(object):

	'''
	All mappers extend this BaseMapper class.

	Mappers expected to contain following methods:
		- map_record():
			- sets self.mapped_record, and returns instance of self
		- as_dict():
			- returns self.mapped_record as dictionary
		- as_json():
			- returns self.mapped_record as json
	'''

	def __init__(self):

		logger.debug('init BaseMapper')



class MODSMapper(BaseMapper):

	'''
	Mapper for MODS metadata

		- flattens MODS with XSLT transformation (derivation of Islandora and Fedora GSearch xslt)
			- field names are combination of prefixes and attributes
		- convert to dictionary with xmltodict
	'''

	def __init__(self):

		# init parent BaseMapper
		# super().__init__()

		# set xslt transformer		
		xslt_tree = etree.parse('/opt/combine/inc/xslt/MODS_extract.xsl')
		self.xsl_transform = etree.XSLT(xslt_tree)


	def map_record(self, record_id, record_string):

		try:
			
			# flatten file with XSLT transformation
			xml_root = etree.fromstring(record_string)
			flat_xml = self.xsl_transform(xml_root)

			# convert to dictionary
			flat_dict = xmltodict.parse(flat_xml)

			# prepare as flat mapped
			fields = flat_dict['fields']['field']
			mapped_dict = { field['@name']:field['#text'] for field in fields }

			# add temporary id field
			mapped_dict['temp_id'] = record_id

			return (
				'success',
				mapped_dict
			)

		except Exception as e:
			
			return (
				'fail',
				{
					'id':row.id,
					'msg':str(e)
				}
			)

