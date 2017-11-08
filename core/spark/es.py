# imports
import django
from elasticsearch import Elasticsearch
import json
from lxml import etree
import os
import re
import requests
import sys
import xmltodict

# import Row from pyspark
from pyspark.sql import Row
from pyspark.sql.types import StringType, IntegerType
from pyspark.sql.functions import udf

# init django settings file to retrieve settings
os.environ['DJANGO_SETTINGS_MODULE'] = 'combine.settings'
sys.path.append('/opt/combine')
django.setup()
from django.conf import settings



class ESIndex(object):

	'''
	Class to organize methods for indexing mapped/flattened metadata into ElasticSearch (ES)
	'''

	@staticmethod
	def index_job_to_es_spark(spark, job, records_df, index_mapper):

		'''
		Method to index records dataframe into ES

		Args:
			job (core.models.Job): Job for records
			records_df (pyspark.sql.DataFrame): records as pyspark DataFrame 
			index_mapper (str): string of indexing mapper to use (e.g. MODSMapper)

		Returns:
			None
				- indexes records to ES
		'''

		# get index mapper
		index_mapper_handle = globals()[index_mapper]

		# create rdd from index mapper
		mapped_records_rdd = records_df.rdd.map(lambda row: index_mapper_handle().map_record(
			row.id, row.document, job.record_group.publish_set_id))

		# attempt to write index mapping failures to DB
		try:
			# filter out index mapping failures
			failures_df = mapped_records_rdd.filter(lambda row: row[0] == 'fail')\
			.map(lambda row: Row(id=row[1]['id'], mapping_error=row[1]['mapping_error'])).toDF()

			# add job_id as column
			job_id = job.id
			job_id_udf = udf(lambda id: job_id, IntegerType())
			failures_df = failures_df.withColumn('job_id', job_id_udf(failures_df.id))

			# write mapping failures to DB
			failures_df.withColumn('record_id', failures_df.id).select(['record_id', 'job_id', 'mapping_error'])\
			.write.jdbc(
					settings.COMBINE_DATABASE['jdbc_url'],
					'core_indexmappingfailure',
					properties=settings.COMBINE_DATABASE,
					mode='append'
				)
		
		except:
			pass

		# retrieve successes to index
		to_index_rdd = mapped_records_rdd.filter(lambda row: row[0] == 'success')

		# create index in advance
		es_handle_temp = Elasticsearch(hosts=[settings.ES_HOST])
		index_name = 'j%s' % job.id
		mapping = {'mappings':{'record':{'date_detection':False}}}
		es_handle_temp.indices.create(index_name, body=json.dumps(mapping))

		# index to ES
		to_index_rdd.saveAsNewAPIHadoopFile(
			path='-',
			outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
			keyClass="org.apache.hadoop.io.NullWritable",
			valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",
			conf={
					"es.resource":"%s/record" % index_name,
					"es.nodes":"192.168.45.10:9200",
					"es.mapping.exclude":"temp_id",
					"es.mapping.id":"temp_id"
				}
		)


	@staticmethod
	def index_published_job(**kwargs):

		'''
		Index published records to ES by copying documents from another index

		Args:
			kwargs
				job_id (int): Job ID
				publish_set_id (str): core.models.RecordGroup.published_set_id, used to build OAI identifier

		Returns:
			None
				- submits POST request to trigger ES to copy documents
		'''

		# copy indexed documents from job to /published
		es_handle_temp = Elasticsearch(hosts=[settings.ES_HOST])
		index_name = 'published'

		# check if published index exists
		if not es_handle_temp.indices.exists(index_name):
			mapping = {'mappings':{'record':{'date_detection':False}}}
			es_handle_temp.indices.create(index_name, body=json.dumps(mapping))

		# prepare _reindex query
		dupe_dict = {
			'source':{
				'index': 'j%s' % kwargs['job_id'],
				'query':{
					'term':{
						'publish_set_id':
						kwargs['publish_set_id']
					}
				}
			},
			'dest': {
				'index':index_name
			}
		}
		r = requests.post('http://%s:9200/_reindex' % settings.ES_HOST,
				data=json.dumps(dupe_dict),
				headers={'Content-Type':'application/json'}
			)



class BaseMapper(object):

	'''
	All mappers extend this BaseMapper class.

	Mappers expected to contain following methods:
		- map_record():
			- sets self.mapped_record, and returns instance of self
	'''

	# def __init__(self):

	# 	logger.debug('init BaseMapper')



class GenericMapper(BaseMapper):

	'''
	Generic flattener of nested, or flat, XML, suitable for indexing in ElasticSearch

	Looping through all elements in an XML tree, the xpath for each element, in combination with attributes is used
	to generate a flattened version of the field into a single string.

	e.g.
	<foo>
		<bar type="geographic">Seattle</bar>
		<bar type="topic">city</bar>
	</foo>
	<foo>
		<baz>Cats Cradle</baz>
		<baz>Breakfast of Champions</baz>
	</foo>


	becomes...

	[
		('foo_bar_type_geographic', ['Seattle']),
		('foo_bar_type_topic', ['city']),
		('foo_bar', ['Cats Cradle','Breakfast of Champions'])
	]

	Args:
		record_id (str): record id
		record_string (str): string of record document
		publish_set_id (str): core.models.RecordGroup.published_set_id, used to build OAI identifier

	Returns:
		(tuple):
			0 (str): ['success','fail']
			1 (dict): details from mapping process, success or failure
	'''

	def __init__(self):

		# empty elems list
		self.flat_elems = []

		# empty formatted elems dict, grouping by flat, formatted element
		self.formatted_elems = {}


	def flatten_record(self):

		'''
		Walk XML tree, writing each element with some basic information
		of xpath, attributes, and text to self.flat_elems()

		Args:
			None

		Returns:
			None
				- sets self.flat_elems
		'''

		# pre-compile checker for blank spaces
		blank_check = re.compile(r"[^ \t\n]")

		# walk descendants of root
		for elem in self.xml_root.iterdescendants():

			# if text value present for element, save to list
			if elem.text and re.search(blank_check, elem.text) is not None:

				# get xpath
				xpath = self.xml_tree.getpath(elem)

				# strip index if repeating
				xpath = re.sub(r'\[[0-9]+\]','',xpath)

				# append
				self.flat_elems.append({
						'text':elem.text,
						'xpath':xpath,
						'attributes':elem.attrib
					})


	def format_record(self, include_attributes=True):

		'''
		After elements have been flattened, with text, xpath, and attributes, 
		derive single string for flattened field, and append potentially repeating
		values to self.formatted_elems

		Args:
			None

		Returns:
			None
				- sets self.formatted_elems
		'''

		# loop through flattened elements
		for elem in self.flat_elems:

			# split on slashes
			xpath_comps = elem['xpath'].lstrip('/').split('/')

			# proceed if not entirely asterisks
			if set(xpath_comps) != set('*'):

				# remove namespaces if present
				for i,comp in enumerate(xpath_comps):
					if ':' in comp:
						xpath_comps[i] = comp.split(':')[-1]

				# remove asterisks from xpath_comps, as they are unhelpful
				xpath_comps = [ c for c in xpath_comps if c != '*' ]

				# if include attributes
				if include_attributes:
					for k,v in elem['attributes'].items():
						xpath_comps.append('%s_%s' % (k,v))

				# self.formatted_elems.append(('_'.join(xpath_comps), elem['text']))

				# derive flat field name
				flat_field = '_'.join(xpath_comps)
				
				# # if not yet seen, add as list to dictionary
				# if flat_field not in self.formatted_elems.keys():
				# 	self.formatted_elems[flat_field] = []

				# # append value to dictionary
				# self.formatted_elems[flat_field].append(elem['text'])

				self.formatted_elems[flat_field] = elem['text']


	def map_record(self, record_id, record_string, publish_set_id):

		# set record string
		self.xml_string = record_string

		# parse from string
		self.xml_root = etree.fromstring(self.xml_string)

		# get tree
		self.xml_tree = self.xml_root.getroottree()

		# flatten record
		self.flatten_record()

		# format for return
		self.format_record()

		# add temporary id field
		self.formatted_elems['temp_id'] = record_id

		# add publish set id
		self.formatted_elems['publish_set_id'] = publish_set_id

		return (
				'success',
				self.formatted_elems
			)



class MODSMapper(BaseMapper):

	'''
	Mapper for MODS metadata

		- flattens MODS with XSLT transformation (derivation of Islandora and Fedora GSearch xslt)
			- field names are combination of prefixes and attributes
		- convert to dictionary with xmltodict
	'''

	def __init__(self):

		# set xslt transformer		
		xslt_tree = etree.parse('/opt/combine/inc/xslt/MODS_extract.xsl')
		self.xsl_transform = etree.XSLT(xslt_tree)


	def map_record(self, record_id, record_string, publish_set_id):

		'''
		Map record

		Args:
			record_id (str): record id
			record_string (str): string of record document
			publish_set_id (str): core.models.RecordGroup.published_set_id, used to build OAI identifier

		Returns:
			(tuple):
				0 (str): ['success','fail']
				1 (dict): details from mapping process, success or failure

		'''

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

			# add publish set id
			mapped_dict['publish_set_id'] = publish_set_id

			return (
				'success',
				mapped_dict
			)

		except Exception as e:
			
			return (
				'fail',
				{
					'id':record_id,
					'mapping_error':str(e)
				}
			)

