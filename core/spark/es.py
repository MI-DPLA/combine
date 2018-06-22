# imports
import django
from elasticsearch import Elasticsearch
import json
from lxml import etree
from lxml.etree import _Element, _ElementUnicodeResult
import os
import re
import requests
import sys
import time
import xmltodict

# pyjxslt
import pyjxslt

# import Row from pyspark
try:
	from pyspark.sql import Row
	from pyspark.sql.types import StringType, IntegerType
	from pyspark.sql.functions import udf
except:
	pass

# check for registered apps signifying readiness, if not, run django.setup() to run as standalone
if not hasattr(django, 'apps'):
	os.environ['DJANGO_SETTINGS_MODULE'] = 'combine.settings'
	sys.path.append('/opt/combine')
	django.setup()	

# import django settings
from django.conf import settings

# import xml2kvp
from core.xml2kvp import XML2kvp


class ESIndex(object):

	'''
	Class to organize methods for indexing mapped/flattened metadata into ElasticSearch (ES)
	'''

	@staticmethod
	def index_job_to_es_spark(spark, job, records_df, index_mapper, include_attributes):

		'''
		Method to index records dataframe into ES

		Args:
			spark (pyspark.sql.session.SparkSession): spark instance from static job methods
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
		def es_mapper_pt_udf(pt):

			# init mapper once per partition
			mapper = index_mapper_handle(include_attributes=include_attributes)

			for row in pt:

				yield mapper.map_record(
					record_string=row.document,
					db_id=row.id,
					combine_id=row.combine_id,
					record_id=row.record_id,				
					publish_set_id=job.record_group.publish_set_id,
					fingerprint=row.fingerprint
				)

		mapped_records_rdd = records_df.rdd.mapPartitions(es_mapper_pt_udf)

		# attempt to write index mapping failures to DB
		# filter our failures
		failures_rdd = mapped_records_rdd.filter(lambda row: row[0] == 'fail')

		# if failures, write
		if not failures_rdd.isEmpty():

			failures_df = failures_rdd.map(lambda row: Row(combine_id=row[1]['combine_id'], mapping_error=row[1]['mapping_error'])).toDF()

			# add job_id as column
			job_id = job.id
			job_id_udf = udf(lambda id: job_id, IntegerType())
			failures_df = failures_df.withColumn('job_id', job_id_udf(failures_df.combine_id))

			# write mapping failures to DB
			failures_df.withColumn('combine_id', failures_df.combine_id).select(['combine_id', 'job_id', 'mapping_error'])\
			.write.jdbc(
					settings.COMBINE_DATABASE['jdbc_url'],
					'core_indexmappingfailure',
					properties=settings.COMBINE_DATABASE,
					mode='append'
				)
		
		# retrieve successes to index
		to_index_rdd = mapped_records_rdd.filter(lambda row: row[0] == 'success')

		# create index in advance
		index_name = 'j%s' % job.id
		es_handle_temp = Elasticsearch(hosts=[settings.ES_HOST])
		if not es_handle_temp.indices.exists(index_name):
			
			# prepare mapping
			mapping = {
				'mappings':{
					'record':{
						'date_detection':False,
						'properties':{
							'combine_db_id':{'type':'integer'},
						}
					}
				}
			}
			
			# create index
			es_handle_temp.indices.create(index_name, body=json.dumps(mapping))

		# index to ES
		to_index_rdd.saveAsNewAPIHadoopFile(
			path='-',
			outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
			keyClass="org.apache.hadoop.io.NullWritable",
			valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",
			conf={
					"es.resource":"%s/record" % index_name,
					"es.nodes":"%s:9200" % settings.ES_HOST,
					"es.mapping.exclude":"temp_id",
					"es.mapping.id":"temp_id",
				}
		)


	@staticmethod
	def copy_es_index(
		source_index=None,
		target_index=None,
		create_target_index=True,
		target_index_mapping={'mappings':{'record':{'date_detection':False}}},
		refresh=True,
		wait_for_completion=True,
		add_copied_from=None):

		'''
		Method to duplicate one ES index to another

		Args:
			create_target_index (boolean): If True, check for target and create
			source_index (str): Source ES index to copy from
			target_index (str): Target ES index to copy to
			target_index_mapping (dict): Dictionary of mapping to create target index

		Returns:
			(dict): results of reindex via elasticsearch client reindex request
		'''

		# get ES handle
		es_handle_temp = Elasticsearch(hosts=[settings.ES_HOST])

		# if creating target index check if target index exists
		if create_target_index and not es_handle_temp.indices.exists(target_index):
			es_handle_temp.indices.create(target_index, body=json.dumps(target_index_mapping))

		# prepare reindex query
		dupe_dict = {
			'source':{
				'index': source_index,
				'query':{}
			},
			'dest': {
				'index':target_index
			}			
		}

		# if add_copied_from, include in reindexed document
		if add_copied_from:
			dupe_dict['script'] = {
				'inline': 'ctx._source.source_job_id = %s' % add_copied_from,
				'lang': 'painless'
			}
		
		# reindex using elasticsearch client
		reindex = es_handle_temp.reindex(body=dupe_dict, wait_for_completion=wait_for_completion, refresh=refresh)
		return reindex



class BaseMapper(object):

	'''
	All mappers extend this BaseMapper class.

	Contains some useful methods and attributes that other mappers may use

	Mappers expected to contain following methods:
		- map_record()
	'''

	# pre-compiled regex
	# checker for blank spaces
	blank_check_regex = re.compile(r"[^ \t\n]")
	# element tag name
	namespace_prefix_regex = re.compile(r'(\{.+\})?(.*)')


	def get_namespaces(self):
		
		'''
		Method to parse namespaces from XML document and save to self.nsmap
		'''

		nsmap = {}
		for ns in self.xml_root.xpath('//namespace::*'):
			if ns[0]:
				nsmap[ns[0]] = ns[1]
		self.nsmap = nsmap

		# set inverted nsmap
		self.nsmap_inv = {v: k for k, v in self.nsmap.items()}



class XML2kvpMapper(BaseMapper):

	'''
	Map XML to ElasticSearch friendly fields with xml2kvp	
	
	Args:
		record_id (str): record id
		record_string (str): string of record document
		publish_set_id (str): core.models.RecordGroup.published_set_id, used to build OAI identifier

	Returns:
		(tuple):
			0 (str): ['success','fail']
			1 (dict): details from mapping process, success or failure
	'''


	# Index Mapper class attributes (needed for easy access in Django templates)
	classname = "XML2kvpMapper" # must be same as class name
	name = "XML2kvp mapper"


	def __init__(self, include_attributes = None):		

		if include_attributes == None:
			self.include_attributes = settings.INCLUDE_ATTRIBUTES_GENERIC_MAPPER
		else:
			self.include_attributes = include_attributes


	def map_record(self,
			record_string=None,
			db_id=None,
			combine_id=None,
			record_id=None,
			publish_set_id=None,
			fingerprint=None
		):

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

			# prepare literals
			literals = {

				# add temporary id field
				'temp_id':combine_id,

				# add combine_id field
				'combine_id':combine_id,

				# add record_id field
				'record_id':record_id,

				# add publish set id
				'publish_set_id':publish_set_id,

				# add record's Combine DB id
				'db_id':db_id,

				# add record's crc32 document hash, aka "fingerprint"
				'fingerprint':fingerprint,

			}

			# map with XML2kvp
			kvp_dict = XML2kvp.xml_to_kvp(
				record_string,
				xml_attribs=self.include_attributes,
				node_delim='___',
				ns_prefix_delim='|',
				copy_to=None,
				literals=literals,
				skip_root=False,
				skip_repeating_values=True,
				skip_attribute_ns_declarations=True,
				error_on_delims_collision=False,
				include_xml_prop=False)

			return (
					'success',
					kvp_dict
				)

		except Exception as e:
			
			return (
				'fail',
				{
					'combine_id':combine_id,
					'mapping_error':str(e)
				}
			)



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
			('foo_bar_type_geographic', 'Seattle'),
			('foo_bar_type_@topic', 'city'),
			('foo_bar', ('Cats Cradle','Breakfast of Champions'))
				# note tuple for multiple values, not list here, as saveAsNewAPIHadoopFile requires
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


	# Index Mapper class attributes (needed for easy access in Django templates)
	classname = "GenericMapper" # must be same as class name
	name = "Generic XPath based mapper"


	def __init__(self, include_attributes = None):

		# empty elems list
		self.flat_elems = []

		# empty formatted elems dict, grouping by flat, formatted element
		self.formatted_elems = {}

		if include_attributes == None:
			self.include_attributes = settings.INCLUDE_ATTRIBUTES_GENERIC_MAPPER
		else:
			self.include_attributes = include_attributes


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

		# reset flat_elems
		self.flat_elems = []

		# walk descendants of root
		for elem in self.xml_root.iterdescendants():

			# if text value present for element, save to list
			if elem.text and re.search(self.blank_check_regex, elem.text) is not None:

				# get xpath
				xpath = self.xml_tree.getpath(elem)

				# append
				self.flat_elems.append({
						'text':elem.text,
						'xpath':xpath,
						'attributes':elem.attrib
					})


	def format_record(self):

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

		# reset formatted elems
		self.formatted_elems = {}

		# loop through flattened elements
		for elem in self.flat_elems:

			# split on slashes
			xpath_comps = elem['xpath'].lstrip('/').split('/')

			# proceed if not entirely asterisks
			if set(xpath_comps) != set('*'):

				# walk parents to develop flat field name
				self.hops = []
				node = self.xml_root.xpath(elem['xpath'], namespaces=self.nsmap)[0]
				self.hops.append(node)

				self.parent_walk(node)

				# reverse order
				self.hops.reverse()

				# loop through hops to develop field name
				fn_pieces = []				
				for hop in self.hops:

					# OLD
					# # add field name, removing etree prefix
					# prefix, tag_name = re.match(self.namespace_prefix_regex, hop.tag).groups()
					# fn_pieces.append(tag_name)

					# NEW 
					# add field name with prefix, e.g. mods|subject
					prefix, tag_name = re.match(self.namespace_prefix_regex, hop.tag).groups()
					try:
						fn_pieces.append('%s|%s' % (self.nsmap_inv[prefix.strip('{}')], tag_name))
					except:
						fn_pieces.append(tag_name)

					# add attributes if not root node
					if self.include_attributes and hop != self.xml_root:					
						attribs = [ (k,v) for k,v in hop.attrib.items() ]
						attribs.sort(key=lambda x: x[0])
						for attribute, value in attribs:						
							if re.search(self.blank_check_regex, value) is not None:								
								attribute = attribute.replace(' ','_')
								prefix, attribute = re.match(self.namespace_prefix_regex, attribute).groups()
								value = value.replace(' ','_')
								fn_pieces.append('@%s=%s' % (attribute,value))

				# derive flat field name
				flat_field = '_'.join(fn_pieces)

				# replace any periods in flat field name with underscore
				flat_field = flat_field.replace('.','_')
				
				# if not yet seen, add to dictionary as single element
				if flat_field not in self.formatted_elems.keys():
					self.formatted_elems[flat_field] = elem['text']

				# elif, field exists, but not yet list, convert to list and append value
				elif flat_field in self.formatted_elems.keys() and type(self.formatted_elems[flat_field]) != list:
					temp_val = self.formatted_elems[flat_field]
					self.formatted_elems[flat_field] = [temp_val, elem['text']]

				# else, if list, and value not already present, append
				else:
					if elem['text'] not in self.formatted_elems[flat_field]:
						self.formatted_elems[flat_field].append(elem['text'])

		# convert all lists to tuples (required for saveAsNewAPIHadoopFile() method)
		for k,v in self.formatted_elems.items():
			if type(v) == list:
				self.formatted_elems[k] = tuple(v)


	def parent_walk(self, node):

		'''
		Small method to walk parents of XML node
		'''

		parent = node.getparent()
		if parent != None:
			self.hops.append(parent)
			self.parent_walk(parent)


	def map_record(self,
			record_string=None,
			db_id=None,
			combine_id=None,
			record_id=None,
			publish_set_id=None,
			fingerprint=None
		):

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

		# set record string, encoded as utf8
		self.xml_string = record_string.encode('utf-8')

		try:

			# parse from string
			self.xml_root = etree.fromstring(self.xml_string)

			# get tree
			self.xml_tree = self.xml_root.getroottree()

			# save namespaces
			self.get_namespaces()

			# flatten record
			self.flatten_record()

			# format for return
			self.format_record()

			# add temporary id field
			self.formatted_elems['temp_id'] = combine_id

			# add combine_id field
			self.formatted_elems['combine_id'] = combine_id

			# add record_id field
			self.formatted_elems['record_id'] = record_id

			# add publish set id
			self.formatted_elems['publish_set_id'] = publish_set_id

			# add record's Combine DB id
			self.formatted_elems['db_id'] = db_id

			# add record's crc32 document hash, aka "fingerprint"
			self.formatted_elems['fingerprint'] = fingerprint

			return (
					'success',
					self.formatted_elems
				)

		except Exception as e:
			
			return (
				'fail',
				{
					'combine_id':combine_id,
					'mapping_error':str(e)
				}
			)



class MODSMapper(BaseMapper):

	'''
	Mapper for MODS metadata

		- flattens MODS with XSLT transformation (derivation of Islandora and Fedora GSearch xslt)
			- field names are combination of prefixes and attributes
		- convert to dictionary with xmltodict
	'''


	# Index Mapper class attributes (needed for easy access in Django templates)
	classname = "MODSMapper" # must be same as class name
	name = "Custom MODS mapper"


	def __init__(self, include_attributes=None, xslt_processor='lxml'):

		'''
		Initiates MODSMapper, with option of what XSLT processor to use.
			- lxml: faster, but does not provide XSLT 2.0 support (though the included stylesheet does not require)
			- pyjxslt: slower, but offers XSLT 2.0 support

		Args:
			xslt_processor (str)['lxml','pyjxslt']: Selects which XSLT processor to use.
		'''

		self.xslt_processor = xslt_processor
		self.xslt_filepath = '/opt/combine/inc/xslt/MODS_extract.xsl'

		if self.xslt_processor == 'lxml':

			# set xslt transformer
			xslt_tree = etree.parse(self.xslt_filepath)
			self.xsl_transform = etree.XSLT(xslt_tree)

		elif self.xslt_processor == 'pyjxslt':

			# prepare pyjxslt gateway
			self.gw = pyjxslt.Gateway(6767)
			with open(self.xslt_filepath,'r') as f:
				self.gw.add_transform('xslt_transform', f.read())


	def map_record(self,
			record_string=None,
			db_id=None,
			combine_id=None,
			record_id=None,
			publish_set_id=None,
			fingerprint=None
		):

		try:
			
			if self.xslt_processor == 'lxml':
				
				# flatten file with XSLT transformation
				xml_root = etree.fromstring(record_string.encode('utf-8'))
				flat_xml = self.xsl_transform(xml_root)

			elif self.xslt_processor == 'pyjxslt':

				# flatten file with XSLT transformation
				flat_xml = self.gw.transform('xslt_transform', record_string)

			# convert to dictionary			
			flat_dict = xmltodict.parse(flat_xml)

			# prepare as flat mapped
			fields = flat_dict['fields']['field']
			mapped_dict = { field['@name']:field['#text'] for field in fields }

			# add temporary id field
			mapped_dict['temp_id'] = combine_id

			# add combine_id field
			mapped_dict['combine_id'] = combine_id

			# add record_id field
			mapped_dict['record_id'] = record_id

			# add publish set id
			mapped_dict['publish_set_id'] = publish_set_id

			# add record's Combine DB id
			mapped_dict['db_id'] = db_id

			# add record's crc32 document hash, aka "fingerprint"
			mapped_dict['fingerprint'] = db_id

			return (
				'success',
				mapped_dict
			)

		except Exception as e:
			
			return (
				'fail',
				{
					'combine_id':combine_id,
					'mapping_error':str(e)
				}
			)



